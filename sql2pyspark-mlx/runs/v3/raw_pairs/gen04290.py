from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(((F.col("country") != "SG") & (F.lower(F.trim("supplier_name")) == "eurogoods gmbh")) | (F.col("country") == "UK"))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
)

suppliers_stats = (
    src
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
)

result = (
    suppliers_stats
    .select("country", "pct_us", "de_rating")
    .orderBy(F.col("de_rating").desc())
)
