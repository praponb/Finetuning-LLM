from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w_part = Window.partitionBy(F.year("delivered_date"), "order_id").orderBy(F.col("cost").desc())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.add_months("shipped_date", 3).alias("shipped_date_plus_months"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w_grp).alias("row_seq"),
        F.cume_dist().over(w_part).alias("cost_cume_dist"),
    )
)
