from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country") == "UK")

suppliers_stats = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = (
    suppliers_stats
    .select("country", "country_values")
    .orderBy(F.col("country_values").desc(), "country")
    .limit(10)
)
