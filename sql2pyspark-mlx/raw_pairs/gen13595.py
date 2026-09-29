from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 6, 7]))
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 250, "High").when(F.col("quantity") >= 39, "Medium").otherwise("Low"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    ranked
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") >= 156)
        .select("id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("quantity_rounded").between(47, 75))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_rounded",
        "quantity_band",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.least("quantity_rounded", "quantity").alias("least_quantity_rounded_quantity"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.city"), F.col("b.store_name"))
)

result = (
    joined
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "product_id",
        "quantity",
        "last_restock_month_end",
        "least_quantity_rounded_quantity",
        "city",
        "store_name",
        F.when(F.col("city").rlike("^(Tokyo|Bangkok)$"), "EXTENDED").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("CORE").alias("city_zone"),
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("last_restock_month_end").desc_nulls_first())
)
