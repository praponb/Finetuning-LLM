from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Tok%"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("bangkok_rating") >= 59)
)
