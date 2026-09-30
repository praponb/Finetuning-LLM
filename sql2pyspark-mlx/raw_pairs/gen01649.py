from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("city").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy(F.col("event_seq").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy("seq_no", "id")
w_running = Window.partitionBy("product_id").orderBy("event_seq", "id")
w_hist = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-03-24").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city").isin(["New York", "Bangkok", "London"]))
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        F.col("p.city"),
        F.col("p.store_name"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.count("quantity").over(w4).alias("store_id_count_quantity"),
    )
)

scored_inventory = (
    scored
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no_2"),
        F.lag("quantity", 1).over(w6).alias("prev_quantity"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "quantity",
        "seq_no_2",
        "prev_quantity",
        F.lag("prev_quantity").over(w_running).alias("prev_prev_quantity"),
        F.lag("seq_no_2").over(w_running).alias("prev_seq_no_2"),
        F.row_number().over(w_running).alias("event_seq_2"),
        F.last("quantity", ignorenulls=True).over(w_hist).alias("last_known_quantity"),
    )
)
