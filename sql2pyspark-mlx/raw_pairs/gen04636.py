from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("attributes").alias("k", "value"))

result = (
    exploded
    .groupBy("k")
    .agg(
        F.size(F.collect_set("k")).alias("num_ks"),
        F.size(F.collect_set("k")).alias("num_ks_2"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.sort_array(F.collect_set("k")).alias("k_list_2"),
        F.size(F.collect_set("k")).alias("num_ks_3"),
    )
)
