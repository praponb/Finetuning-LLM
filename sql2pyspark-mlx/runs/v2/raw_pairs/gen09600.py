from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-01-12", "2024-04-01"))
    .select(
        "id",
        "product_id",
        "units",
        "sale_date",
        "revenue",
        F.round((F.col("units") / F.sum("units").over(w)), 3).alias("units_share"),
        F.lead("revenue", 2).over(w2).alias("next_revenue"),
    )
)
