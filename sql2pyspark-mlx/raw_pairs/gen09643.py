from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("least_amount_discount", F.least("amount", "discount"))
    .withColumn("order_date_label", F.date_format("order_date", "yyyy-MM-dd"))
    .orderBy("id")
)
