from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy(F.col("salary").asc_nulls_last())
w3 = Window.partitionBy("location").orderBy(F.col("salary").desc_nulls_first(), "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.level"))
)

ranked = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped
    .filter(F.col("location").isNotNull() & (F.col("dept_name") == "Finance"))
    .withColumn("adjusted_salary", F.round((F.col("salary") * 1.15), 2))
)

result = (
    base
    .withColumn("budget_tier", F.when(F.col("budget") >= 3145000, "HIGH").otherwise("LOW"))
    .rollup("budget_tier")
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("location") == "London", F.col("salary"))).alias("london_avg_salary"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("london_avg_salary") > 0)
    .orderBy(F.col("budget_tier").desc())
)
