from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("units")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

with_windows = (
    df_store_sales
    .withColumn("next_revenue", F.lead("revenue").over(w_hist))
    .withColumn("next_units", F.lead("units").over(w_hist))
    .withColumn("prev_units", F.lag("units").over(w_hist))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("running_units", F.sum("units").over(w2))
)

matched_keys = (
    with_windows
    .select("store_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id") == 6)
        .select("store_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.region").isin(["North", "East", "West"]))
    .select("src.*", F.col("s.opened_date"), F.col("s.store_name"), F.col("s.region"))
)

base = (
    enriched
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-08-02"))) & F.col("region").isNotNull())
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "next_revenue",
        "next_units",
        "prev_units",
        "row_seq",
        "opened_date",
        "store_name",
        "region",
        (F.col("units") + F.col("next_units")).alias("units_plus_next_units"),
    )
)

scored = base.withColumn("units_rank", F.rank().over(w3))

result = (
    scored
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "sale_date",
        "units",
        "next_revenue",
        "next_units",
        "opened_date",
        "store_name",
        "region",
        "units_plus_next_units",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Downtown Express)$"), "GROUP_B").when(F.col("store_name").rlike("^(Siam Flagship|Mall Central)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("store_name_zone"),
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("next_revenue").asc_nulls_first())
)
