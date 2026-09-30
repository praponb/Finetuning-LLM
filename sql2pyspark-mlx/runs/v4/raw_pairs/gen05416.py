from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.last_restock") <= "2025-08-25")
    .select(
        F.col("i.quantity"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.weekofyear(F.col("ss.sale_date")).alias("sale_date_weekofyear"),
    )
    .orderBy(F.col("sale_date_weekofyear").desc())
)
