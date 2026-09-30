from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("country") == "JP") & F.col("supplier_name").like("Gl%")) | F.col("supplier_name").like("%City%"))
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("max_rating") >= 2.7)
    .orderBy(F.col("city_values").desc())
)
