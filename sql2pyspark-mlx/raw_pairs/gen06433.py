from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.sale_date") <= "2024-08-01") & (F.col("ss.units") < 16))
    .select(
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("ss.units"),
        F.year(F.col("ss.sale_date")).alias("sale_date_year"),
    )
    .orderBy(F.col("sale_date_year").desc(), "units")
)
