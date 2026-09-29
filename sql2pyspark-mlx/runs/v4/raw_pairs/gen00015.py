from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"), "category")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.countDistinct("category").alias("distinct_categories"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("cost_per_globex"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
    )
)
