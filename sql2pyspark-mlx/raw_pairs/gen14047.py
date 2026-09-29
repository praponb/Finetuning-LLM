from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-11").cast("date"))
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3300, "High").when(F.col("revenue") >= 900, "Medium").otherwise("Low"))
)

metrics = (
    base
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue_tier",
        F.lead("units").over(w).alias("next_units"),
        F.lag("units").over(w).alias("prev_units"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.row_number().over(w).alias("event_seq"),
        F.ntile(4).over(w2).alias("units_bucket"),
    )
)

valid = (
    metrics
    .filter((F.col("sale_date") > F.lit("2023-04-09").cast("date")) & F.col("store_id").isin([4, 6]))
    .withColumn("next_revenue_int", F.col("next_revenue").cast("int"))
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "sale_date",
        "prev_units",
        "prev_revenue",
        "next_revenue",
        "event_seq",
        "units_bucket",
        "next_revenue_int",
    )
)

joined = (
    ranked
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") >= "2023-08-01")
    .select("e.*", F.col("s.city"), F.col("s.opened_date"))
)

result = (
    joined
    .select(
        "id",
        "store_id",
        "prev_units",
        "prev_revenue",
        "next_revenue",
        "units_bucket",
        "next_revenue_int",
        "city",
        "opened_date",
    )
    .orderBy("prev_units")
)
