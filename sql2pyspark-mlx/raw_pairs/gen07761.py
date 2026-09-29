from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("rating") >= 3.4)

agg = (
    filtered_suppliers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)

result = (
    agg
    .filter(F.col("rating_range") > 50)
    .select("city", "pct_th", "rating_range")
    .orderBy("rating_range")
)
