from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.last("cost").over(w).alias("last_cost"),
    )
    .orderBy(F.col("delivered_date").desc())
)
