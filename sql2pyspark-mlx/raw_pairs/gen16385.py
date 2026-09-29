from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.orderBy("dept_name_length", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy("budget_filled", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("location")
w6 = Window.partitionBy("dept_name").orderBy("location_avg_budget", "id").rowsBetween(-3, -1)

valid = (
    df_departments
    .filter(F.col("dept_name").eqNullSafe(F.lit("Finance")))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("N/A")))
)

cleaned = (
    valid
    .filter(F.col("budget") > 2313000)
    .select(
        "*",
        F.length("dept_name").alias("dept_name_length"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "dept_name",
        "location",
        "budget_filled",
        F.stddev_samp("budget").over(w).alias("dept_name_stddev_samp_budget"),
        F.avg("budget").over(w2).alias("budget_avg_2_prior"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
        F.avg("dept_name_length").over(w4).alias("dept_name_length_avg_5"),
        F.avg("budget").over(w5).alias("location_avg_budget"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "dept_name",
        "location",
        "budget_filled",
        "dept_name_stddev_samp_budget",
        "budget_avg_2_prior",
        "budget_share",
        "location_avg_budget",
        F.min("dept_name_length_avg_5").over(w5).alias("location_min_dept_name_length_avg_5"),
        F.avg("budget_avg_2_prior").over(w6).alias("budget_avg_2_prior_avg_3_prior"),
    )
)

kept_keys = (
    metrics
    .select("dept_name", "location")
    .exceptAll(
        df_departments
        .filter(F.col("location") != "Tokyo")
        .select("dept_name", "location")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "dept_name",
        "budget_avg_2_prior",
        "budget_share",
        "location_avg_budget",
        "location_min_dept_name_length_avg_5",
        "budget_avg_2_prior_avg_3_prior",
    )
    .orderBy("budget_share")
)
