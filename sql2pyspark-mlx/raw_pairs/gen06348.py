from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("days_since_last_restock_minus_quantity")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

filtered = (
    base
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-09-29")))
    .select(
        "*",
        F.least("quantity", "days_since_last_restock").alias("least_quantity_days_since_last_restock"),
        (F.col("days_since_last_restock") - F.col("quantity")).alias("days_since_last_restock_minus_quantity"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") <= "2023-12-01")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.quantity",
        "src.days_since_last_restock",
        "src.days_since_last_restock_minus_quantity",
        F.col("b.sqft"),
        F.col("b.city"),
    )
)

result = (
    enriched
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "days_since_last_restock",
        "days_since_last_restock_minus_quantity",
        "city",
        F.when(F.col("city").rlike("^(Chiang Mai|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Bangkok|New York)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
        F.col("max_inventory_quantity"),
    )
)
