from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_filled").asc_nulls_first())
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy(F.col("budget_filled").desc())

filtered = (
    df_departments
    .filter((F.col("dept_name").eqNullSafe(F.lit("Engineering"))) & (F.col("budget") < 3933000))
    .withColumn("budget_band", F.when(F.col("budget") >= 3835000, "High").when(F.col("budget") >= 1797000, "Medium").otherwise("Low"))
)

matched_keys = (
    filtered
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget") <= 2783000)
        .select("location", "dept_name")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget_band",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)

eligible_departments = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.full_name").isNotNull()), "left_anti")
)

enriched = (
    eligible_departments
    .withColumn("budget_filled_dense_rank", F.dense_rank().over(w))
    .withColumn("location_avg_budget_filled", F.avg("budget_filled").over(w2))
    .withColumn("budget_filled_rank", F.rank().over(w3))
    .withColumn("location_avg_budget_filled_2", F.avg("budget_filled").over(w2))
    .withColumn("location_avg_budget_filled_3", F.avg("budget_filled").over(w2))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("c.email"))) == "jack@startup.io"), "left_anti")
)

result = (
    not_flagged
    .select(
        "budget_filled",
        "budget_filled_dense_rank",
        "location_avg_budget_filled",
        "location_avg_budget_filled_2",
        "location_avg_budget_filled_3",
    )
    .orderBy(F.col("location_avg_budget_filled_3").desc())
)
