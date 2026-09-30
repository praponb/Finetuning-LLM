from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") > "2023-12-01")
    .select("*", F.ceil("sqft").alias("sqft_whole"))
)

valid = (
    base
    .filter(F.col("region").isNotNull())
    .select("*", F.length("region").alias("region_length"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_anti")
)

not_flagged_stores = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged_stores
    .groupBy(F.when(F.col("sqft") >= 14797, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy("sqft_tier")
)
