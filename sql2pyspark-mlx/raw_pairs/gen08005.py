from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("delivered_date_plus_months", "id")
w2 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-08-13").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 107, "High").when(F.col("cost") >= 43, "Medium").otherwise("Low").alias("cost_band"),
        F.add_months("delivered_date", 3).alias("delivered_date_plus_months"),
        F.coalesce("carrier", F.lit("OTHER")).alias("resolved_carrier"),
    )
)

result = (
    valid
    .select(
        "id",
        "carrier",
        "status",
        "cost_band",
        "delivered_date_plus_months",
        "resolved_carrier",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("cost").over(w).alias("next_cost"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
)
