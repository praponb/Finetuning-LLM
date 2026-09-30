from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.orderBy("row_num", "id")
w3 = Window.partitionBy("level").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("salary") >= 159000, "High").when(F.col("salary") >= 63000, "Medium").otherwise("Low").alias("salary_band"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.location"))
)

with_windows = (
    with_dims
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w3))
)

result = (
    with_windows
    .withColumn("quarter_start", F.date_trunc("quarter", "hire_date"))
    .rollup("quarter_start")
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("row_seq"))).alias("singapore_total_row_seq"),
        F.grouping_id().alias("grouping_level"),
    )
)
