from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "product_id",
        F.lead("units", 1).over(w).alias("next_units"),
        F.rank().over(w2).alias("sale_date_rank"),
    )
)
