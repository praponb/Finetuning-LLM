from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location")

valid = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
)

matched_keys = (
    valid
    .select("location")
    .subtract(
        df_departments
        .filter(~F.col("dept_name").isin(["Finance", "HR", "Sales"]))
        .select("location")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("dept_name").isNotNull() & F.col("location").isNull())
    .select(
        "location",
        "budget",
        "location_dept_name_key",
        F.when(F.col("budget") >= 3786000, "High").when(F.col("budget") >= 1752000, "Medium").otherwise("Low").alias("budget_tier"),
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("location", F.lit("OTHER")).alias("resolved_location"),
    )
)

result = (
    cleaned
    .withColumn("budget_cume_dist", F.cume_dist().over(w))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("location_min_budget", F.min("budget").over(w2))
)
