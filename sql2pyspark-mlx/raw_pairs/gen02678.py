from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("is_tokyo").desc(), "id")
w_part = Window.partitionBy("location").orderBy("is_tokyo", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location", "job_title").orderBy("salary", "id")
w5 = Window.partitionBy("location", "job_title").orderBy("is_tokyo", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("location").isin(["Singapore", "Bangkok"]))
    .select("*", F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"))
)

latest_match = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.is_tokyo", F.col("c.salary"), F.col("c.job_title"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") > 75000), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "is_tokyo",
        "job_title",
        F.row_number().over(w2).alias("row_seq"),
        F.sum("is_tokyo").over(w_part).alias("cumulative_is_tokyo"),
        F.lag("salary", 2).over(w4).alias("prev_salary"),
        F.sum("salary").over(w_part).alias("cumulative_salary"),
        F.sum("is_tokyo").over(w5).alias("cumulative_is_tokyo_2"),
    )
)

result = (
    with_windows
    .groupBy("job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("job_title").desc_nulls_first())
)
