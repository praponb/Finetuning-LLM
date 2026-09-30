from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

result = (
    latest_match
    .groupBy(F.date_trunc("QUARTER", "store_sale_sale_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
    )
    .filter(F.col("num_cities") > 1)
    .orderBy(F.col("num_cities").desc())
)
