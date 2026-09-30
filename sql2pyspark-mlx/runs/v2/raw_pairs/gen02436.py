from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([1, 2, 3]))
    .select(
        "id",
        "store_id",
        "units",
        "sale_date",
        "revenue",
        F.percent_rank().over(w).alias("units_pct_rank"),
        F.sum("revenue").over(w2).alias("running_revenue"),
    )
    .orderBy("store_id")
)
