from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("prev_budget").desc(), "id")

valid = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "London"]))
    .select("*", F.substring("location", 1, 2).alias("location_prefix"))
)

with_windows = (
    valid
    .select(
        "id",
        "dept_name",
        "location",
        "location_prefix",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "location", "prev_budget")
)

remaining_keys = (
    deduped
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("dept_name").isin(["HR", "Sales"]))
        .select("id", "location")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("location").isNotNull())
    .withColumn("location_filled", F.coalesce("location", F.lit("Unknown")))
    .withColumn("location_clean", F.coalesce("location", F.lit("NONE")))
)

valid_departments = (
    base
    .filter(F.col("location") == "Tokyo")
    .select(
        "id",
        "location",
        "location_filled",
        "location_clean",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)

result = (
    valid_departments
    .groupBy("location")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)
