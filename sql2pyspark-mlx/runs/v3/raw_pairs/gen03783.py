from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-05-01")
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

agg = (
    recent_inventory
    .groupBy(
        F.quarter("last_restock_plus_months").alias("last_restock_plus_months_quarter"),
    )
    .agg(
        F.count("*").alias("num_inventory"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    agg
    .filter(F.col("quantity_range") > 50)
    .select("last_restock_plus_months_quarter", "num_inventory", "quantity_range")
)
