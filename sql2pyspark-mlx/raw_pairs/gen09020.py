from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")

ranked = (
    df_departments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "location")
)

kept_keys = (
    ranked
    .select("location", "id")
    .intersectAll(
        df_departments
        .filter(F.col("budget") < 3871000)
        .select("location", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.location") == F.col("k.location")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("location") == "Singapore")
    .select("*", F.substring("location", 1, 2).alias("location_prefix"))
)

filtered_departments = (
    filtered
    .filter(F.col("location") != "Chiang Mai")
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

kept = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary") < 101000), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("location").isin(["New York", "Singapore", "Tokyo"]))
    .select(
        "id",
        "location",
        "location_prefix",
        "resolved_location",
        F.substring("location", 1, 4).alias("location_prefix_2"),
    )
)

kept_keys_2 = (
    cleaned
    .select("id", "location")
    .subtract(
        df_departments
        .filter(F.col("budget") > 2201000)
        .select("id", "location")
    )
)

verified_departments = (
    cleaned
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    verified_departments
    .groupBy("location")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.countDistinct("location").alias("distinct_locations"),
        F.countDistinct("location").alias("distinct_locations_2"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.size(F.collect_set("location")).alias("num_locations_2"),
    )
    .filter(F.col("num_locations_2") > 1)
    .orderBy(F.col("distinct_locations").desc())
)
