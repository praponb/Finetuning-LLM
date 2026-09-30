from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "TH", "US"]) & F.col("supplier_name").isNotNull())
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
    .orderBy(F.col("country").desc())
)
