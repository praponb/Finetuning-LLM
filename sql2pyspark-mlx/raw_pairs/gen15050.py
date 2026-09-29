from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
    )
)
