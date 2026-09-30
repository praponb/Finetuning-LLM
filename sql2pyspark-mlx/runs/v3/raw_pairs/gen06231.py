from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Sia%") & (F.col("opened_date") > "2025-09-10"))
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Bangkok"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)
