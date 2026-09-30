from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(-6, -1)

result = (
    df_store_sales
    .select(
        "*",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lead("units").over(w).alias("next_units"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("units").over(w2).alias("units_avg_6_prior"),
    )
)
