from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("quantity_whole", "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-17").cast("date")) & (F.col("product_id") == 8))
    .select(
        "id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

scored = (
    valid
    .filter(F.col("quantity_whole") < 56)
    .select(
        "id",
        "last_restock",
        F.lead("quantity_whole").over(w_running).alias("next_quantity_whole"),
        F.lag("quantity_whole").over(w_running).alias("prev_quantity_whole"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

base = (
    scored
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-21")))
    .select("*", F.add_months("last_restock", 12).alias("last_restock_plus_months"))
)

matched_keys = (
    base
    .select("id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") == 127)
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "last_restock", "last_restock_plus_months")
    .orderBy("id")
)
