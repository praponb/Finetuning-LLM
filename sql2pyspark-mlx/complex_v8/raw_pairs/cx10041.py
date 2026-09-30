from pyspark.sql import functions as F

filtered_policys = (
    df_policies
    .filter((F.col("holder_id") >= 9) | (F.col("status").isin("ACTIVE", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
    )
)

labeled = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

result = (
    labeled
    .cube("holder_id", "product")
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "median_premium",
        "total_premium",
        "n_rows",
    )
    .orderBy(F.col("holder_id").asc_nulls_last(), F.col("product").asc_nulls_last())
)
