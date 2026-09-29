from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("units", "id")
w_hist = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-5, -1)

result = (
    df_store_sales
    .select(
        "*",
        F.lag("revenue").over(w_recent).alias("prev_revenue"),
        F.lead("revenue").over(w_recent).alias("next_revenue"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("units").over(w_hist).alias("units_avg_5_prior"),
    )
)
