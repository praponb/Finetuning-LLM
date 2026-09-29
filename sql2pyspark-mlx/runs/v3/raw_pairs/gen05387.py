from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "lion city imports") & F.lower("supplier_name").like("%lob%") & (F.col("rating") < 2.6))
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)
