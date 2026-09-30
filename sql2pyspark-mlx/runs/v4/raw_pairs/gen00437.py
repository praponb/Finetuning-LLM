from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())
w2 = Window.partitionBy("dept_name").orderBy("budget_int", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.when(F.col("budget") >= 4357000, "High").when(F.col("budget") >= 2002000, "Medium").otherwise("Low").alias("budget_band"),
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_clean"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    ranked
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Legal", "Sales"]), "Tier 1").otherwise("Other"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

with_windows = (
    valid
    .select(
        "*",
        F.avg("budget_int").over(w2).alias("budget_int_avg_5"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title").isin(["Manager", "Engineer"])), "left_anti")
)

result = (
    kept
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "Singapore", "Berlin"])
    .agg(F.avg("budget_int").alias("value_sum"))
    .select(
        "dept_name",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Singapore").alias("singapore"),
        F.col("Berlin").alias("berlin"),
    )
)
