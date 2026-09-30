from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "London"]) & F.col("country").isin(["TH", "SG", "DE"]))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
)

summary = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
)

result = (
    summary
    .filter(F.col("jp_rating") >= 40)
    .select("country", "jp_rating")
    .orderBy("country")
    .limit(10)
)
