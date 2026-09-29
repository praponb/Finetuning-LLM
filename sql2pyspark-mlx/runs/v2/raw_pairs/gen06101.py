from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(630, 920))
    .groupBy("brand")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.countDistinct("cost").alias("unique_suppliers"),
    )
)
