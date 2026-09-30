from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select("id", "store_name", "city", "region", "opened_date", "sqft")
    .orderBy(F.col("region").desc())
)
