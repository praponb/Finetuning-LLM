from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at"), "k").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.partitionBy("customer_id", "k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "created_at",
        "k",
        F.row_number().over(w).alias("row_seq"),
        F.last("k", ignorenulls=True).over(w2).alias("last_known_k"),
        F.first("k", ignorenulls=True).over(w3).alias("first_known_k"),
    )
)
