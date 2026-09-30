from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy("cost", "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

enriched = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.lag("cost", 1).over(w2).alias("prev_cost"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    enriched
    .select("id", "carrier", "delivered_date", "status", "last_known_cost", "row_seq")
    .orderBy(F.col("carrier").asc_nulls_first(), "status")
)
