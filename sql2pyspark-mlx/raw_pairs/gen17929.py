from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "product_id").orderBy(F.col("launch_date").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy("years_since_last_restock", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("years_since_last_restock").desc(), "id")

base = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-06-17"))) & (F.col("last_restock") <= "2024-08-27"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        (F.datediff(F.current_date(), "last_restock") / 365.25).alias("years_since_last_restock"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.years_since_last_restock",
        F.col("b.store_name"),
    )
)

joined_inventory = (
    joined
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"), F.col("p.launch_date"), F.col("p.price"))
)

deduped = (
    joined_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "years_since_last_restock",
        "store_name",
    )
)

with_windows = (
    deduped
    .select(
        "id",
        "store_id",
        "years_since_last_restock",
        F.lag("years_since_last_restock").over(w_running).alias("prev_years_since_last_restock"),
        F.row_number().over(w_running).alias("row_seq"),
        F.ntile(5).over(w3).alias("years_since_last_restock_bucket"),
    )
)

result = (
    with_windows
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("dim.region").alias("region"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.row_seq")).alias("max_row_seq"),
        F.grouping_id().alias("gid"),
    )
)
