from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)
