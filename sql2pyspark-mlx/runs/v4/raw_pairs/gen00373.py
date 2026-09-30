from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.1) & (F.col("country") == "DE") & F.col("supplier_name").like("%Siam%"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)
