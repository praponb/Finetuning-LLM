from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("units", "id")

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5, 6]))
    .select("*", (F.col("revenue") + F.col("units")).alias("revenue_per_unit"))
)

result = (
    valid
    .select(
        "id",
        "store_id",
        "revenue",
        F.lag("units").over(w_part).alias("prev_units"),
        F.lead("units").over(w_part).alias("next_units"),
        F.lead("revenue").over(w_part).alias("next_revenue"),
        F.lag("revenue").over(w_part).alias("prev_revenue"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)
