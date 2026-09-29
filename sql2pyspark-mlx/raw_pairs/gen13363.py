from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isNotNull() & F.col("supplier_name").like("%Par%")) | F.col("supplier_name").isNotNull())
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)
