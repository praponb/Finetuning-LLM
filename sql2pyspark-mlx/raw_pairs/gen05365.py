from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level").orderBy("latest_rank", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-07").cast("date")) & F.col("level").isNotNull())
    .select("*", F.length("full_name").alias("full_name_length"))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") > 2858000)
    .select(
        "s.id",
        "s.dept_id",
        "s.is_remote",
        "s.level",
        "s.full_name_length",
        "s.rn",
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("d.location"),
    )
)

result = (
    with_dims
    .groupBy("dept_id")
    .agg(
        F.count_if(F.col("level") == "L2").alias("l2_events"),
        F.sum(F.when(F.col("level") == "L1", F.col("full_name_length"))).alias("l1_total_full_name_length"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.max(F.when(F.col("level") == "L1", F.col("full_name_length"))).alias("l1_max_full_name_length"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
    )
    .filter(F.col("singapore_count") > 1)
)
