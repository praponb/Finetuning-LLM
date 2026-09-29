from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 3)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.sum("units").over(w).alias("running_units"),
    )
    .orderBy("id")
)
