from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter(F.col("status") != "CANCELLED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.trim(F.col("product"))).alias("product_clean"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

derived = (
    prepared_policys
    .withColumn("product_slug_ref", F.concat(F.col("product_slug"), F.lit("#"), F.col("id").cast("string")))
)

result = (
    derived
    .groupBy("status")
    .pivot("product", ["HEALTH", "HOME"])
    .agg(F.sum("premium"))
    .orderBy("status")
)
