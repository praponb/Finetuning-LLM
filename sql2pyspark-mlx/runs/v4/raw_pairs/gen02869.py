from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 100), 2).alias("budget_adjusted"),
        F.when(F.col("dept_name") == "Sales", 1).otherwise(0).alias("is_sales"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.manager_id").isin([5, 8, 16, 37])), "left_semi")
)

base = (
    eligible
    .filter(F.col("budget_adjusted").isNotNull())
    .withColumn("location_length", F.length("location"))
)

valid = (
    base
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "Sales"))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_filled",
        "location_length",
        F.trim("location").alias("clean_location"),
    )
)

summary = (
    valid
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "London", F.col("location_length")).otherwise(0)).alias("london_location_length"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("location_length"), 2).alias("avg_location_length"),
    )
)

result = (
    summary
    .select(
        "location",
        "num_rows",
        "location_list",
        "avg_location_length",
        F.when(F.col("location").rlike("^(Singapore|Chiang Mai)$"), "PRIORITY").otherwise("GROUP_A").alias("location_zone"),
    )
)
