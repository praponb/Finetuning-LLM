from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("next_quantity_adjusted").asc_nulls_last())

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-29"))) & (F.col("quantity") >= 228))
    .select("*", F.round((F.col("quantity") * 1.07), 2).alias("quantity_adjusted"))
)

joined = (
    valid
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.store_name"), F.col("d.region"), F.col("d.sqft"))
)

with_windows = (
    joined
    .filter(F.col("region") != "Central")
    .withColumn("prev_quantity", F.lag("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity").over(w))
    .withColumn("next_quantity_adjusted", F.lead("quantity_adjusted").over(w))
    .withColumn("prev_quantity_adjusted", F.lag("quantity_adjusted").over(w))
    .withColumn("event_seq", F.row_number().over(w))
)

valid_inventory = (
    with_windows
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_adjusted",
        "store_name",
        "sqft",
        "prev_quantity",
        "next_quantity",
        "next_quantity_adjusted",
        "prev_quantity_adjusted",
        "event_seq",
        F.when(F.col("prev_quantity") >= 199, "High").when(F.col("prev_quantity") >= 78, "Medium").otherwise("Low").alias("prev_quantity_level"),
    )
)

deduped = (
    valid_inventory
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    deduped
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([2, 3]))
        .select("product_id", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("product_id", "next_quantity_adjusted", "prev_quantity_adjusted")
    .unpivot(
        ids=["product_id"],
        values=["next_quantity_adjusted", "prev_quantity_adjusted"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
