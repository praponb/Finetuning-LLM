from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("i.product_id") == 3) & ~F.col("s.city").isin(["London", "Bangkok"]))
    .select(
        F.col("i.store_id"),
        F.col("s.city"),
        F.date_trunc("week", F.col("s.opened_date")).alias("opened_date_period"),
    )
    .orderBy("opened_date_period")
)
