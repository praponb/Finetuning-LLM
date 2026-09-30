from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.avg(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_avg_sqft"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
    )
    .filter(F.col("east_rows") > 1)
    .orderBy(F.col("year_start").desc_nulls_last())
)
