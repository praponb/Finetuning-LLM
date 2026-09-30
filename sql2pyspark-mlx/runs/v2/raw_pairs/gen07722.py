from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Fashion") & ((F.col("launch_date") <= "2024-07-18") | F.col("supplier_id").isin([15, 39])))
    .groupBy("category", "brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("cost").alias("unique_suppliers"),
    )
    .filter(F.col("unique_suppliers") >= 5)
)
