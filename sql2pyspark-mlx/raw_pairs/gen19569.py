from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("city") == "Bangkok")

suppliers_stats = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = suppliers_stats.orderBy("city")
