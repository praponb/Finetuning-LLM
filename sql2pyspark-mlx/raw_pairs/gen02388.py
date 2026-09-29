from pyspark.sql import functions as F

result = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 3), "left_anti")
)
