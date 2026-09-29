from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.location", F.col("c.level"))
)

cleaned = (
    latest_match
    .filter(F.col("dept_name").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Finance"))))
    .withColumn("is_hr", F.when(F.col("dept_name") == "HR", 1).otherwise(0))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([15, 29, 31])), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isNotNull()), "left_semi")
)

result = (
    not_flagged
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("is_hr")).otherwise(0)).alias("l2_is_hr"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_rows"),
        F.round(F.avg("is_hr"), 2).alias("avg_is_hr"),
        F.count("*").alias("row_count"),
    )
)
