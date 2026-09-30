from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-07-01")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.lead("units", 2).over(w).alias("next_units"),
    )
    .orderBy("sale_date", F.col("id").desc())
)
