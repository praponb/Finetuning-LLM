from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNull())
    .select(
        "*",
        F.when(F.col("budget") >= 3053000, "High").when(F.col("budget") >= 735000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "location", "budget", "budget_tier")
)

deduped_departments = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_last = (
    deduped_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.location", "s.budget", F.col("c.salary"))
)

kept_keys = (
    with_last
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Tokyo", "Singapore", "London"]))
        .select("id")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("location")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") < 1000000)
    .orderBy(F.col("location").desc())
)
