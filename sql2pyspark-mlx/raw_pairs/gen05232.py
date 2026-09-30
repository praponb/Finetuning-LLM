from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("revenue", "id")

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue").over(w_recent).alias("next_revenue"),
        F.lag("units").over(w_recent).alias("prev_units"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"), F.col("b.region"), F.col("b.opened_date"))
)

joined_store_sales = (
    joined
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") == "Singapore")
    .select(
        "a.id",
        "a.store_id",
        "a.units",
        "a.next_revenue",
        "a.prev_units",
        "a.next_units",
        "a.sqft",
        F.col("p.sqft").alias("store_sqft"),
    )
)

with_dims = (
    joined_store_sales
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.next_revenue",
        "e.prev_units",
        "e.next_units",
        "e.sqft",
        F.col("d.region"),
    )
)

summary = (
    with_dims
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_rows"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
        F.round(F.sum("next_units") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("next_units_per_north"),
        F.percentile_approx("next_units", 0.95).alias("next_units_p95"),
    )
)

result = (
    summary
    .select("region", "north_rows", "next_units_per_north", "next_units_p95")
    .orderBy("region", "north_rows")
)
