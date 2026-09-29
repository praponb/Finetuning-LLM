from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

kept_keys = (
    df_departments
    .select("dept_name", "location")
    .intersectAll(
        df_departments
        .filter(F.col("budget") <= 1910000)
        .select("dept_name", "location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

enriched = kept_rows.select("dept_name", "location", F.rank().over(w).alias("budget_rank"))

valid = (
    enriched
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_group", F.when(F.col("location").isin(["Tokyo", "Bangkok"]), "Group A").otherwise("Other"))
)

cleaned = (
    valid
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.length("dept_name").alias("dept_name_length"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

result = (
    cleaned
    .select(
        "dept_name",
        "location",
        "location_group",
        "dept_name_length",
        "location_filled",
    )
)
