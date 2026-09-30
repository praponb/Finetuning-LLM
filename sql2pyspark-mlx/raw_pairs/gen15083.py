from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("ship_city", "customer_id").orderBy("bulk_lines", "id")
w2 = Window.partitionBy("customer_id", "ship_city").orderBy(F.col("bulk_lines").asc_nulls_last())

parsed = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "bulk_lines",
        "ship_city",
        F.lag("bulk_lines").over(w_hist).alias("prev_bulk_lines"),
        F.lead("bulk_lines").over(w_hist).alias("next_bulk_lines"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.rank().over(w2).alias("bulk_lines_rank"),
    )
)
