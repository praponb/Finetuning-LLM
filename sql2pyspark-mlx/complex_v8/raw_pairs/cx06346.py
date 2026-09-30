from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"))
    .select("sku_id", "move_type")
)

sku_rows = (
    df_skus
    .filter(F.col("weight_kg") < 20.9)
    .select("id", "sku_code")
)

all_events = (
    move_side
    .union(sku_rows)
    .distinct()
)

result = (
    all_events
    .groupBy("move_type")
    .agg(F.count("*").alias("n_rows"), F.sum("sku_id").alias("total_sku_id"))
    .orderBy("move_type")
)
